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
        CLOSED,

        /**
         * The generation was stopped part way for a newer one (STAGES.md
         * §15.8.6); its end says so.
         */
        INTERRUPTED,

        /**
         * A newer generation of the item was there first (STAGES.md
         * §15.8.3): nothing was done.
         */
        SUPERSEDED
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
        if (CorrectionStore.generational(STAGE, pending.seq()))
        {
            return correctAsGeneration(pending, digest, refs, lease);
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
     * The items whose splitting has ended one of whose failures changed
     * since their end was sent (an operator's decline or reopen), for a
     * pod to send their ends again with {@link #reannounce} (STAGES.md
     * §15.8.10 step 5).
     *
     * @param limit the most returned
     * @return their ids; none if the processor has no fail store
     */
    public List<UUID> changedEnds(int limit)
    {
        return failures == null ? List.of()
                : pipeline.changedEnds(STAGE, limit);
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
        int generation = pipeline.checkpoint(itemId)
                .map(Checkpoint::generation).orElse(1);
        // Only a revision of the item's own generation, as corrections of
        // it were before they made generations (STAGES.md §15.8.5).
        return failures != null && failures.failures(itemId).stream()
                .anyMatch(stored -> stored.failure().seq()
                        == FailureStore.WHOLE_ITEM
                        && stored.failure().lineage().generation()
                                == generation
                        && stored.state() == FailureStore.State.SUPERSEDED
                        && !stored.failure().lineage().first());
    }

    /**
     * Takes note that an item is an exact copy of an earlier one, as its
     * {@code item.landed} says (STAGES.md §15.5a): keeps the earlier item,
     * so each end sent for the copy names it, and flags the earlier item's
     * open failures here resent. The copy itself is split in full, as any
     * item is. Noting it again does nothing more.
     *
     * @param itemId the copy
     * @param sameAs the earlier item
     */
    public void noteCopy(UUID itemId, UUID sameAs)
    {
        pipeline.noteSameAs(itemId, sameAs);
        if (failures != null)
        {
            failures.markResent(sameAs, itemId);
        }
    }

    /**
     * The earlier item an item is an exact copy of, as noted by
     * {@link #noteCopy}.
     *
     * @param itemId the item
     * @return the earlier item; empty if it is no copy, or none was noted
     */
    public Optional<UUID> sameAs(UUID itemId)
    {
        return pipeline.sameAs(itemId);
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
        return pipeline.failedRecordCount(itemId, generationOf(itemId));
    }

    /**
     * An item's current generation at this stage (STAGES.md §15.8).
     *
     * @param itemId the item
     * @return its checkpoint's generation; 1 if it was never started
     */
    private int generationOf(UUID itemId)
    {
        return pipeline.checkpoint(itemId).map(Checkpoint::generation)
                .orElse(1);
    }

    /**
     * Reads an item's stored records that carry an error.
     *
     * @param itemId the item
     * @return them, in sequence order
     */
    public List<CommittedRecords.Failed> failedRecords(UUID itemId)
    {
        return pipeline.failedRecords(itemId, generationOf(itemId));
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
        return traced(itemId, () -> run(itemId, new Landing(digest, refs),
                opener, lease, null));
    }

    /**
     * Splits an item again as its next generation, for a reprocess run
     * (STAGES.md §15.8.4): the generation before is finished first if its
     * cut is over, or interrupted if not (§15.8.6); then the new one is
     * cut from the start of the item as landed, under the version running
     * now. A generation already started is a repeat: it is resumed, or its
     * end sent again. One older than the item's is superseded, and nothing
     * is done.
     *
     * @param itemId     the item's ID
     * @param digest     the item's digest, from landing
     * @param opener     opens the item's bytes, if they are needed
     * @param lease      the item's lease, held by the caller
     * @param refs       what landing kept about the item for its result
     * @param generation the generation to cut, from 2
     * @param runId      the run that asked for it
     * @return how the generation ended
     * @throws Exception if storage or the broker fails; the reprocess is
     *                   tried again later
     */
    public Outcome reprocess(UUID itemId, String digest, Opener opener,
            LeaseToken lease, LandedRefs refs, int generation, UUID runId)
            throws Exception
    {
        Checkpoint.Cut cut = new Checkpoint.Cut(generation,
                ConnectorMessage.Cause.REPROCESS, runId, 0);
        return traced(itemId, () -> generation(itemId,
                new Landing(digest, refs), opener, lease, cut));
    }

    /** Work on an item that gives how it ended. */
    @FunctionalInterface
    private interface Work
    {

        /**
         * Does it.
         *
         * @return how it ended
         * @throws Exception if it fails
         */
        Outcome run() throws Exception;
    }

    /**
     * Does work on an item in its trace span, counted as an item in
     * progress.
     *
     * @param itemId the item
     * @param work   the work
     * @return how it ended
     * @throws Exception if the work fails
     */
    private Outcome traced(UUID itemId, Work work) throws Exception
    {
        Telemetry telemetry = Telemetry.get();
        Span span = telemetry.span("ingest.item",
                Fields.of(Attr.ITEM_ID, itemId)
                        .with(Attr.FORMAT, pipeline.settings().format()));
        telemetry.itemStartedProcessing();
        try (Scope ignored = span.makeCurrent())
        {
            Outcome outcome = work.run();
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
     * Takes an item on from wherever its checkpoint is, or starts a
     * generation of it past the checkpoint's.
     *
     * @param itemId  the item's ID
     * @param landing what came with it from landing
     * @param opener  opens its bytes, or, for a generation cut from a
     *                correction, the correction's
     * @param lease   its lease
     * @param cut     a generation to start, past the checkpoint's, whose
     *                generation before has ended; null to go on with the
     *                item's own
     * @return how it ended
     * @throws Exception if storage or the broker fails
     */
    private Outcome run(UUID itemId, Landing landing, Opener opener,
            LeaseToken lease, Checkpoint.Cut cut) throws Exception
    {
        Optional<ItemCheckpoints.Stored> stored = pipeline.stored(itemId);
        boolean fresh = cut != null && (stored.isEmpty()
                || stored.get().checkpoint().generation()
                        < cut.generation());
        Opener source = opener;
        if (stored.isPresent() && !fresh)
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
                case INTERRUPTED ->
                {
                    announce(checkpoint, landing, endedAt);
                    return new Outcome(itemId, Result.INTERRUPTED,
                            checkpoint.recordCount(), null);
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
                    // split (again) below, from what its generation is
                    // cut from.
                    source = sourceOf(checkpoint, opener);
                }
            }
        }
        if (fresh)
        {
            started(itemId, cut);
        }
        IngestPipeline.SplitResult split;
        long size = 0;
        try (SeekableByteSource bytes = source.open())
        {
            size = bytes.size();
            split = pipeline.split(bytes, itemId, lease, landing.refs(),
                    fresh ? cut : null);
        }
        catch (ItemHeldException held)
        {
            return new Outcome(itemId, Result.HELD, 0, null);
        }
        catch (ItemInterruptedException interrupted)
        {
            return interrupt(pipeline.checkpoint(itemId).orElseThrow(),
                    landing, lease, interrupted.newer());
        }
        catch (Exception failure)
        {
            if (!permanent(failure))
            {
                throw failure;
            }
            return fail(failingCheckpoint(itemId, size, failure,
                    fresh ? cut : null), landing, lease);
        }
        return finishCompletion(split.checkpoint(), landing, lease);
    }

    /**
     * Starts an item's next generation (STAGES.md §15.8.4), or goes on
     * with it if started: the generation before is finished if its cut is
     * over, interrupted if it is part way or held (§15.8.6), and its end
     * sent again if it was interrupted already, in case a pod stopped
     * before sending it; then the new one is cut.
     *
     * @param itemId  the item
     * @param landing what came with it from landing
     * @param opener  opens what the generation is cut from
     * @param lease   its lease
     * @param cut     the generation
     * @return how the generation ended; {@code SUPERSEDED} if the item is
     *         past it
     * @throws Exception if storage or the broker fails
     */
    private Outcome generation(UUID itemId, Landing landing, Opener opener,
            LeaseToken lease, Checkpoint.Cut cut) throws Exception
    {
        Optional<ItemCheckpoints.Stored> stored = pipeline.stored(itemId);
        if (stored.isPresent())
        {
            Checkpoint current = stored.get().checkpoint();
            if (current.generation() > cut.generation())
            {
                return new Outcome(itemId, Result.SUPERSEDED, 0, null);
            }
            if (current.generation() == cut.generation())
            {
                return run(itemId, landing, opener, lease, null);
            }
            switch (current.state())
            {
                case SPLITTING, HELD -> interrupt(pipeline.resume(itemId,
                        lease).orElseThrow(), landing, lease,
                        cut.generation());
                case ARCHIVING -> finishCompletion(current, landing, lease);
                case FAILING -> finishFailure(current, landing, lease);
                case INTERRUPTED -> announce(current, landing,
                        stored.get().endedAt());
                default ->
                {
                    // Ended: its end was sent.
                }
            }
        }
        return run(itemId, landing, opener, lease, cut);
    }

    /**
     * Notes a generation started (STAGES.md §15.8.3): the failures of the
     * generations before are superseded, and
     * {@code stage.generation_started} is audited. Done again, changing
     * nothing more, if the pod stops before the generation's checkpoint
     * is written.
     *
     * @param itemId the item
     * @param cut    the generation
     */
    private void started(UUID itemId, Checkpoint.Cut cut)
    {
        if (cut.generation() == 1)
        {
            return;
        }
        if (failures != null)
        {
            failures.supersedeBefore(itemId, cut.generation());
        }
        Telemetry.get().audit("stage.generation_started",
                Fields.of(Attr.STAGE, STAGE).with(Attr.ITEM_ID, itemId)
                        .with(Attr.GENERATION, cut.generation())
                        .with(Attr.CAUSE, cut.cause())
                        .with(Attr.RUN_ID, cut.runId()));
    }

    /**
     * Ends a generation part way for a newer one (STAGES.md §15.8.6): any
     * records committed but not acknowledged are sent, the checkpoint is
     * {@code INTERRUPTED}, audited {@code split.generation_interrupted}
     * and counted, and the generation's end sent, saying it was
     * interrupted.
     *
     * @param current the generation's checkpoint, any batch part way
     *                settled
     * @param landing what came with the item from landing
     * @param lease   its lease
     * @param newer   the generation that stopped it
     * @return how it ended
     * @throws Exception if storage or the broker fails
     */
    private Outcome interrupt(Checkpoint current, Landing landing,
            LeaseToken lease, int newer) throws Exception
    {
        if (current.committedSeq() > current.publishedSeq())
        {
            pipeline.announceCommitted(current, landing.refs());
        }
        Checkpoint ended = current.published(current.committedSeq())
                .interrupted();
        pipeline.commitCheckpoint(ended, lease);
        Telemetry telemetry = Telemetry.get();
        telemetry.auditWarn("split.generation_interrupted",
                Fields.of(Attr.ITEM_ID, ended.itemId())
                        .with(Attr.GENERATION, ended.generation())
                        .with(Attr.SUPERSEDED_BY, "g" + newer)
                        .with(Attr.COUNT, ended.recordCount())
                        .with(Attr.OFFSET, ended.resumeAt().offset()));
        telemetry.generationInterrupted();
        Ended announced = announce(ended, landing,
                endedAt(ended.itemId()));
        auditEnded(announced, Result.INTERRUPTED);
        return new Outcome(ended.itemId(), Result.INTERRUPTED,
                ended.recordCount(), null);
    }

    /**
     * What a generation part way is cut from: the item as landed, or, for
     * one cut from a correction of the item as a whole (STAGES.md
     * §15.8.5), the correction's content, still pending.
     *
     * @param current the generation's checkpoint
     * @param landed  opens the item as landed
     * @return what opens it
     * @throws IllegalStateException if the correction isn't pending
     */
    private Opener sourceOf(Checkpoint current, Opener landed)
    {
        int correction = current.cut().correction();
        if (correction == 0)
        {
            return landed;
        }
        if (corrections == null)
        {
            throw new IllegalStateException("No correction store");
        }
        CorrectionStore.Pending pending = corrections.find(
                current.itemId(), FailureStore.WHOLE_ITEM, correction)
                .orElseThrow(() -> new IllegalStateException("The"
                        + " correction the generation is cut from isn't"
                        + " pending"));
        return () -> pipeline.correctionSource(pending);
    }

    /**
     * Applies an operator's correction or retry of an item as a whole
     * (STAGES.md §15.8.5): it is cut as the item's next generation, the
     * number noted when it was submitted, from its content (a retry's is
     * the item as landed), and passes if the cut reaches the end. One whose
     * generation is older than the item's, since a reprocess came first,
     * isn't cut: it ends {@code CORRECTION_SUPERSEDED}, its edit kept. A
     * failure of the generation is the stage's own, recorded at its end,
     * so none is added for the correction.
     *
     * @param pending the correction
     * @param digest  the item's digest, from landing
     * @param refs    what landing kept about the item
     * @param lease   the item's lease
     * @return what it came to
     * @throws Exception if storage or the broker fails
     */
    private CorrectionStore.Outcome correctAsGeneration(
            CorrectionStore.Pending pending, String digest, LandedRefs refs,
            LeaseToken lease) throws Exception
    {
        UUID itemId = pending.itemId();
        Checkpoint current = pipeline.checkpoint(itemId)
                .orElseThrow(() -> new IllegalStateException(
                        "The item was never split"));
        int generation = pending.lineage() != null
                && pending.lineage().generation() > 1
                        ? pending.lineage().generation()
                        : current.generation() + 1;
        CorrectionStore.Outcome outcome;
        if (current.generation() > generation)
        {
            outcome = new CorrectionStore.Outcome(pending, false, null,
                    ErrorCode.CORRECTION_SUPERSEDED.name(), version());
        }
        else
        {
            Checkpoint.Cut cut = new Checkpoint.Cut(generation,
                    IngestPipeline.cause(pending), null,
                    pending.correction());
            Outcome ended = generation(itemId, new Landing(digest, refs),
                    () -> pipeline.correctionSource(pending), lease, cut);
            boolean passed = ended.result() == Result.COMPLETED;
            String code = passed ? null
                    : ended.failure() != null ? ended.failure().code().name()
                    : ErrorCode.CORRECTION_SUPERSEDED.name();
            outcome = new CorrectionStore.Outcome(pending, passed, null,
                    code, version());
        }
        corrections.finish(outcome, false);
        corrections.markNotified(itemId);
        return outcome;
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
     * steps are safe to repeat, so after a crash it is done again. Once
     * sent, the end is noted with when its failures were read, so a pod
     * sends it again if one changes after (STAGES.md §15.8.10 step 5).
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
        List<CommittedRecords.Row> failed = pipeline.failedRows(itemId,
                checkpoint.generation());
        FailureStore.Counts counts;
        Instant readAt = null;
        if (failures == null)
        {
            counts = SplitMessages.unstored(checkpoint, failed.size());
        }
        else
        {
            failures.record(SplitMessages.failures(checkpoint, failed));
            // Before the counts are read, so a change after is sent again.
            readAt = pipeline.databaseNow();
            counts = failures.counts(itemId);
        }
        SplitMessages.Corrected corrected = corrections == null
                ? SplitMessages.Corrected.NONE
                : SplitMessages.Corrected.of(checkpoint,
                        pipeline.correctionRows(itemId,
                                checkpoint.generation()),
                        wholeItemCorrected(itemId));
        // Numbered before it is sent (STAGES.md §15.6e).
        // An interrupted generation's end is a completion of its own.
        int completion = pipeline.announced(itemId, counts.open() == 0
                || checkpoint.state() == Checkpoint.State.INTERRUPTED);
        ConnectorMessage message = SplitMessages.itemEnd(checkpoint, intake,
                failed.size(), counts, landing.digest(), landing.refs(),
                endedAt, pipeline.clock().instant(), corrected, completion);
        pipeline.publishNow(List.of(message));
        if (readAt != null)
        {
            pipeline.markAnnounced(itemId, readAt);
        }
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
     * @param cut     the generation being started, if its checkpoint may
     *                not be written yet; null for the item's own
     * @return the checkpoint
     */
    private Checkpoint failingCheckpoint(UUID itemId, long size,
            Exception failure, Checkpoint.Cut cut)
    {
        Instant now = pipeline.clock().instant();
        Checkpoint base = pipeline.checkpoint(itemId)
                .filter(current -> current.state().inProgress()
                        && (cut == null
                                || current.generation() == cut.generation()))
                .orElseGet(() -> Checkpoint.start(itemId,
                        IngestPipeline.itemFingerprint(itemId),
                        pipeline.settings().format(), size, now,
                        cut == null ? null : pipeline.settings().version(),
                        cut == null ? Checkpoint.Cut.FIRST : cut));
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
