/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.stage;

import static java.nio.charset.StandardCharsets.US_ASCII;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.sdh.healthcare.connector.broker.receive.BatchHandler;
import com.sdh.healthcare.connector.broker.receive.BrokerMessage;
import com.sdh.healthcare.connector.contract.ContractVersion;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage.ItemConverted;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage.ItemLanded;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage.ItemProgressed;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage.RecordStored;
import com.sdh.healthcare.connector.contract.message.Lineage;
import com.sdh.healthcare.connector.contract.message.PayloadCodec;
import com.sdh.healthcare.connector.core.failure.CorrectionStore;
import com.sdh.healthcare.connector.core.failure.FailureStore;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

/**
 * Tests for {@link StageWorker}, on a real Postgres, with real sealing and
 * a processor that stores record ids.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
class StageWorkerTest
{

    /** How the workers run: claims short enough to test expiry. */
    static final StageWorker.Settings SETTINGS = new StageWorker.Settings(4,
            Duration.ofSeconds(30), Duration.ofSeconds(5),
            Duration.ofMillis(50), Duration.ofSeconds(5));

    /** The audit events. */
    @RegisterExtension
    final AuditCapture audit = new AuditCapture();

    /** Every record committed, over all workers. */
    final List<String> committed =
            Collections.synchronizedList(new ArrayList<>());

    /** The stage's output, shared by all workers. */
    final StageTestSupport.RecordingPublisher output =
            new StageTestSupport.RecordingPublisher();

    /** The workers made, closed after each test. */
    final List<StageWorker<String>> workers = new ArrayList<>();

    /** The registry's schema for this test. */
    String schema;

    /** The item the records are from. */
    final UUID item = UUID.randomUUID();

    /** Creates the tests; JUnit runs each test on a new instance. */
    StageWorkerTest()
    {
    }

    /** Gives each test its own tables. */
    @BeforeEach
    void tables()
    {
        schema = StageTestSupport.freshSchema();
    }

    /** Closes the workers. */
    @AfterEach
    void close()
    {
        for (StageWorker<String> worker : workers)
        {
            worker.close();
        }
    }

    /**
     * A worker.
     *
     * @param owner     its name in the claims
     * @param processor its processor
     * @return the worker, started
     */
    StageWorker<String> worker(String owner,
            StageTestSupport.TestProcessor processor)
    {
        StageWorker<String> worker = new StageWorker<>(processor,
                StageTestSupport.registry(schema, owner),
                StageTestSupport.stageOpener(),
                StageTestSupport.stageSealer(), output, SETTINGS);
        worker.start();
        workers.add(worker);
        return worker;
    }

    /**
     * A processor at version 1, committing to this test's list.
     *
     * @return the processor
     */
    StageTestSupport.TestProcessor processor()
    {
        return new StageTestSupport.TestProcessor(1, committed);
    }

    /**
     * An item's records, with its total last.
     *
     * @param records how many
     * @return the batch
     */
    List<BrokerMessage> batch(int records)
    {
        List<BrokerMessage> batch = new ArrayList<>();
        for (int seq = 0; seq < records; seq++)
        {
            batch.add(StageTestSupport.record(item, seq));
        }
        batch.add(StageTestSupport.total(item, records));
        return batch;
    }

    /**
     * The record messages sent, by record id. A message sent again has the
     * same id, and the stage after drops it, so each id is listed once.
     *
     * @return the ids, in the order first sent
     */
    List<String> sentRecords()
    {
        return output.opened().stream()
                .filter(RecordStored.class::isInstance)
                .map(message -> ((RecordStored) message).recordId())
                .distinct().toList();
    }

    /**
     * The item-complete messages sent.
     *
     * @return the messages, in order
     */
    List<ItemConverted> sentItems()
    {
        return output.opened().stream()
                .filter(ItemConverted.class::isInstance)
                .map(ItemConverted.class::cast).toList();
    }

    /**
     * The item-progress messages sent.
     *
     * @return the messages, in order
     */
    List<ItemProgressed> sentProgress()
    {
        return output.opened().stream()
                .filter(ItemProgressed.class::isInstance)
                .map(ItemProgressed.class::cast).toList();
    }

    /**
     * A record that fails is stored and kept in the fail store, once, and
     * not sent on; its item isn't complete while the failure is open, and
     * gets item.progress instead, once for its counts (STAGES.md §15.6).
     *
     * @throws InterruptedException if interrupted
     */
    @Test
    void keepsAFailureAndReportsItsItemInProgress()
            throws InterruptedException
    {
        StageTestSupport.TestProcessor processor = processor();
        StageWorker<String> worker = worker("pod-a", processor);
        assertThat(worker.handle(batch(5)))
                .isEqualTo(BatchHandler.Outcome.DONE);
        assertThat(committed).hasSize(5);
        assertThat(sentRecords()).hasSize(4)
                .doesNotContain("rec-" + item + "-4");
        assertThat(sentItems()).isEmpty();
        assertThat(sentProgress()).singleElement().satisfies(progress ->
        {
            assertThat(progress.itemId()).isEqualTo(item);
            assertThat(progress.recordCount()).isEqualTo(5);
            assertThat(progress.passedCount()).isEqualTo(4);
            assertThat(progress.openCount()).isEqualTo(1);
        });
        assertThat(processor.progressed).singleElement().satisfies(
                progress -> assertThat(progress.complete()).isFalse());
        assertThat(processor.completed).isEmpty();
        assertThat(audit.names()).containsExactly("failure.failed",
                "item.progress");
        FailureStore failures = StageTestSupport.registry(schema, "pod-a")
                .failureStore();
        assertThat(failures.failures(item)).singleElement().satisfies(
                stored ->
                {
                    assertThat(stored.state())
                            .isEqualTo(FailureStore.State.FAILED);
                    assertThat(stored.failure().seq()).isEqualTo(4);
                    assertThat(stored.failure().errorCode())
                            .isEqualTo("TEST_FAILED");
                    assertThat(stored.failure().version()).isEqualTo("v1");
                });

        // Delivered again: a duplicate, with nothing new to report.
        assertThat(worker.handle(batch(5)))
                .isEqualTo(BatchHandler.Outcome.DONE);
        assertThat(committed).hasSize(5);
        assertThat(sentProgress()).hasSize(1);
        assertThat(failures.counts(item))
                .isEqualTo(new FailureStore.Counts(1, 0, 0));
    }

    /**
     * Once an operator declines an item's last open failure, the worker's
     * revisit finds the item changed and completes it; a revisit with
     * nothing changed sends nothing (STAGES.md §15.3).
     *
     * @throws InterruptedException if interrupted
     */
    @Test
    void completesAnItemOnceItsFailureIsDeclined() throws InterruptedException
    {
        StageTestSupport.TestProcessor processor = processor();
        StageWorker<String> worker = worker("pod-a", processor);
        assertThat(worker.handle(batch(5)))
                .isEqualTo(BatchHandler.Outcome.DONE);
        assertThat(sentProgress()).hasSize(1);
        worker.revisit();
        assertThat(sentProgress()).hasSize(1);
        assertThat(sentItems()).isEmpty();

        FailureStore failures = StageTestSupport.registry(schema, "pod-a")
                .failureStore();
        failures.change(item, 4, Set.of(FailureStore.State.FAILED),
                FailureStore.State.DECLINED, "declined", "alice", null);
        worker.revisit();
        assertThat(sentItems()).singleElement().satisfies(done ->
        {
            assertThat(done.itemId()).isEqualTo(item);
            assertThat(done.recordCount()).isEqualTo(5);
        });
        // The producer had the item's result once, in progress; the
        // decline sends it nothing (STAGES.md §15.8.7).
        assertThat(processor.progressed).hasSize(1);
        assertThat(processor.completed).isEmpty();

        // Complete: not revisited again.
        worker.revisit();
        assertThat(sentItems()).hasSize(1);
        assertThat(failures.counts(item))
                .isEqualTo(new FailureStore.Counts(0, 1, 0));
    }

    /**
     * An item complete here whose failure an operator reopens is
     * incomplete again (STAGES.md §15.6e): the worker reports it in
     * progress, reopening its first completion, and once the failure is
     * declined again completes it a second time, under a new message ID.
     *
     * @throws InterruptedException if interrupted
     */
    @Test
    void completesAgainAnItemWhoseFailureIsReopened()
            throws InterruptedException
    {
        StageTestSupport.TestProcessor processor = processor();
        StageWorker<String> worker = worker("pod-a", processor);
        assertThat(worker.handle(batch(5)))
                .isEqualTo(BatchHandler.Outcome.DONE);
        FailureStore failures = StageTestSupport.registry(schema, "pod-a")
                .failureStore();
        failures.change(item, 4, Set.of(FailureStore.State.FAILED),
                FailureStore.State.DECLINED, "declined", "alice", null);
        worker.revisit();
        assertThat(sentItems()).singleElement().satisfies(done ->
                assertThat(done.completion()).isEqualTo(1));
        String first = sentItems().get(0).messageId();

        failures.change(item, 4, Set.of(FailureStore.State.DECLINED),
                FailureStore.State.FAILED, "reopened", "alice", null);
        worker.revisit();
        assertThat(audit.names()).contains("stage.item_reopened");
        assertThat(sentProgress()).last().satisfies(progress ->
        {
            assertThat(progress.completion()).isEqualTo(1);
            assertThat(progress.openCount()).isEqualTo(1);
            assertThat(progress.messageId()).endsWith("~c1");
        });
        assertThat(sentItems()).hasSize(1);

        failures.change(item, 4, Set.of(FailureStore.State.FAILED),
                FailureStore.State.DECLINED, "declined", "alice", null);
        worker.revisit();
        assertThat(sentItems()).hasSize(2).last().satisfies(done ->
        {
            assertThat(done.completion()).isEqualTo(2);
            assertThat(done.messageId()).isNotEqualTo(first)
                    .endsWith("~c2");
        });
        // Reported to the producer once, in progress, before any operator
        // action (STAGES.md §15.8.7).
        assertThat(processor.progressed).hasSize(1);
        assertThat(processor.completed).isEmpty();

        // Complete again: nothing more.
        worker.revisit();
        assertThat(sentItems()).hasSize(2);
    }

    /**
     * An operator's correction is processed by a worker (STAGES.md
     * §15.6c) as the next revision of its record's place (§15.8.5): one
     * that fails supersedes the failure, and is a failure of its own, open,
     * so the item stays incomplete, sending nothing; one that passes is
     * stored and sent on with its revision, supersedes the failure, and the
     * item completes with every record passed. The producer's result is
     * sent only for the item's first report, not after the correction. A
     * second correction while one is pending is refused.
     *
     * @throws InterruptedException if interrupted
     */
    @Test
    void appliesAnOperatorsCorrection() throws InterruptedException
    {
        StageTestSupport.TestProcessor processor = processor();
        StageWorker<String> worker = worker("pod-a", processor);
        assertThat(worker.handle(batch(5)))
                .isEqualTo(BatchHandler.Outcome.DONE);
        StageRegistry registry = StageTestSupport.registry(schema, "pod-a");
        CorrectionStore corrections = registry.correctionStore();
        FailureStore failures = registry.failureStore();

        assertThat(corrections.submit(item, 4, "still bad".getBytes(US_ASCII),
                StageTestSupport.CORRECTION_KEYS,
                StageTestSupport.CORRECTION_KEK, "alice")).isEqualTo(1);
        assertThatThrownBy(() -> corrections.submit(item, 4,
                "again".getBytes(US_ASCII), StageTestSupport.CORRECTION_KEYS,
                StageTestSupport.CORRECTION_KEK, "alice"))
                .isInstanceOf(FailureStore.RefusedException.class);
        worker.applyCorrections();
        worker.revisit();
        Lineage second = Lineage.FIRST.nextRevision(Lineage.CONVERT);
        assertThat(failures.failure(item, 4)).get().satisfies(stored ->
        {
            assertThat(stored.state()).isEqualTo(FailureStore.State.FAILED);
            assertThat(stored.failure().lineage()).isEqualTo(second);
        });
        assertThat(failures.failures(item)).extracting(
                FailureStore.Stored::state).containsExactlyInAnyOrder(
                        FailureStore.State.FAILED,
                        FailureStore.State.SUPERSEDED);
        assertThat(sentRecords()).hasSize(4);
        assertThat(sentItems()).isEmpty();
        assertThat(audit.names()).contains("failure.correction_submitted",
                "failure.correction_failed");

        assertThat(corrections.submit(item, 4, "fixed".getBytes(US_ASCII),
                StageTestSupport.CORRECTION_KEYS,
                StageTestSupport.CORRECTION_KEK, "alice")).isEqualTo(2);
        worker.applyCorrections();
        worker.revisit();
        assertThat(sentRecords()).hasSize(5)
                .contains("rec-" + item + "-4-c2");
        assertThat(failures.failure(item, 4)).get()
                .extracting(FailureStore.Stored::state)
                .isEqualTo(FailureStore.State.SUPERSEDED);
        assertThat(failures.failures(item)).extracting(
                FailureStore.Stored::state).containsOnly(
                        FailureStore.State.SUPERSEDED);
        assertThat(corrections.list(item, 4)).extracting(
                CorrectionStore.Listed::state).containsExactly(
                        CorrectionStore.State.FAILED,
                        CorrectionStore.State.PASSED);
        assertThat(corrections.list(item, 4).get(1).recordId())
                .isEqualTo("rec-" + item + "-4-c2");
        assertThat(sentItems()).singleElement().satisfies(done ->
        {
            assertThat(done.recordCount()).isEqualTo(5);
            assertThat(done.failedRecordCount()).isZero();
        });
        // Reported to the producer once, in progress, before the
        // corrections (STAGES.md §15.8.7).
        assertThat(processor.progressed).hasSize(1);
        assertThat(processor.completed).isEmpty();

        // Nothing left: a later interval does nothing more.
        worker.applyCorrections();
        worker.revisit();
        assertThat(sentRecords()).hasSize(5);
        assertThat(sentItems()).hasSize(1);
    }

    /**
     * A retry (STAGES.md §15.6e) is processed as a correction, the next
     * revision of its place (§15.8.5): one that fails supersedes the
     * failure, and its own failure is the one open, with the retry's
     * error kept on its row; one that passes supersedes that, and
     * completes the item.
     *
     * @throws InterruptedException if interrupted
     */
    @Test
    void appliesARetry() throws InterruptedException
    {
        StageTestSupport.TestProcessor processor = processor();
        StageWorker<String> worker = worker("pod-a", processor);
        assertThat(worker.handle(batch(5)))
                .isEqualTo(BatchHandler.Outcome.DONE);
        StageRegistry registry = StageTestSupport.registry(schema, "pod-a");
        CorrectionStore corrections = registry.correctionStore();
        FailureStore failures = registry.failureStore();

        assertThat(corrections.submit(item, 4, "still bad".getBytes(US_ASCII),
                StageTestSupport.CORRECTION_KEYS,
                StageTestSupport.CORRECTION_KEK, "alice",
                CorrectionStore.Kind.RETRY)).isEqualTo(1);
        worker.applyCorrections();
        worker.revisit();
        assertThat(failures.failure(item, 4)).get()
                .extracting(FailureStore.Stored::state)
                .isEqualTo(FailureStore.State.FAILED);
        assertThat(corrections.list(item, 4)).singleElement()
                .satisfies(retry ->
                {
                    assertThat(retry.kind())
                            .isEqualTo(CorrectionStore.Kind.RETRY);
                    assertThat(retry.state())
                            .isEqualTo(CorrectionStore.State.FAILED);
                    assertThat(retry.errorCode()).isNotNull();
                });
        assertThat(sentItems()).isEmpty();
        assertThat(audit.names()).contains("failure.retry_submitted",
                "failure.retry_failed");

        assertThat(corrections.submit(item, 4, "same".getBytes(US_ASCII),
                StageTestSupport.CORRECTION_KEYS,
                StageTestSupport.CORRECTION_KEK, "alice",
                CorrectionStore.Kind.RETRY)).isEqualTo(2);
        worker.applyCorrections();
        worker.revisit();
        assertThat(failures.failure(item, 4)).get().satisfies(stored ->
        {
            assertThat(stored.state())
                    .isEqualTo(FailureStore.State.SUPERSEDED);
            assertThat(stored.failure().lineage()).isEqualTo(Lineage.FIRST
                    .nextRevision(Lineage.CONVERT));
        });
        assertThat(sentRecords()).contains("rec-" + item + "-4-c2");
        assertThat(audit.names()).contains("failure.retried");
        assertThat(sentItems()).singleElement().satisfies(done ->
                assertThat(done.failedRecordCount()).isZero());
    }

    /**
     * An item the stage before still has failures open for comes with its
     * item.progress: the records passed so far are handled, and the item
     * reported in progress, but not completed until its final total comes
     * (STAGES.md §15.6).
     *
     * @throws InterruptedException if interrupted
     */
    @Test
    void waitsForTheStageBeforeToCompleteAnItem() throws InterruptedException
    {
        StageTestSupport.TestProcessor processor = processor();
        StageWorker<String> worker = worker("pod-a", processor);
        List<BrokerMessage> partial = new ArrayList<>();
        for (int seq = 0; seq < 3; seq++)
        {
            partial.add(StageTestSupport.record(item, seq));
        }
        partial.add(StageTestSupport.received(new ItemProgressed(item,
                "convert", "before", 4, 3, 1, 0, null, null, null,
                java.time.Instant.EPOCH, null)));
        assertThat(worker.handle(partial))
                .isEqualTo(BatchHandler.Outcome.DONE);
        assertThat(sentItems()).isEmpty();
        assertThat(sentProgress()).singleElement().satisfies(progress ->
        {
            assertThat(progress.passedCount()).isEqualTo(3);
            assertThat(progress.openCount()).isZero();
        });
        assertThat(processor.progressed).singleElement().satisfies(
                progress -> assertThat(progress.upstreamOpen()).isTrue());

        // The stage before resolved its failure: one more record, and its
        // final total.
        assertThat(worker.handle(List.of(StageTestSupport.record(item, 3),
                StageTestSupport.total(item, 4))))
                .isEqualTo(BatchHandler.Outcome.DONE);
        assertThat(sentItems()).singleElement().satisfies(done ->
                assertThat(done.recordCount()).isEqualTo(4));
        // Its result went to the producer in progress; what the stage
        // before's operator resolved sends it nothing (STAGES.md §15.8.7).
        assertThat(processor.completed).isEmpty();
    }

    /**
     * A batch is processed, committed once, sent, and its item completed
     * with its counts.
     *
     * @throws InterruptedException if interrupted
     */
    @Test
    void storesSendsAndCompletesAnItem() throws InterruptedException
    {
        StageWorker<String> worker = worker("pod-a", processor());
        assertThat(worker.handle(batch(4)))
                .isEqualTo(BatchHandler.Outcome.DONE);
        assertThat(committed).hasSize(4).doesNotHaveDuplicates();
        assertThat(sentRecords()).containsExactlyInAnyOrderElementsOf(
                committed);
        assertThat(sentItems()).singleElement().satisfies(done ->
        {
            assertThat(done.itemId()).isEqualTo(item);
            assertThat(done.recordCount()).isEqualTo(4);
            assertThat(done.failedRecordCount()).isZero();
        });
        assertThat(sentProgress()).isEmpty();
        assertThat(audit.names()).containsExactly("stage.item_completed");
        assertThat(audit.attributes("stage.item_completed"))
                .containsEntry("sdh.stage", "convert")
                .containsEntry("sdh.result", "COMPLETED")
                .containsKey("sdh.count")
                .containsKey("sdh.failed_count");
    }

    /**
     * An item's total coming before its records, in another batch, and a
     * record twice in one batch, change nothing.
     *
     * @throws InterruptedException if interrupted
     */
    @Test
    void takesTheTotalBeforeTheRecords() throws InterruptedException
    {
        StageWorker<String> worker = worker("pod-a", processor());
        assertThat(worker.handle(List.of(StageTestSupport.total(item, 2))))
                .isEqualTo(BatchHandler.Outcome.DONE);
        assertThat(sentItems()).isEmpty();
        assertThat(worker.handle(List.of(StageTestSupport.record(item, 0),
                StageTestSupport.record(item, 0),
                StageTestSupport.record(item, 1))))
                .isEqualTo(BatchHandler.Outcome.DONE);
        assertThat(committed).hasSize(2);
        assertThat(sentItems()).hasSize(1);
    }

    /**
     * A batch delivered again to another worker of the same version is
     * recognised without processing it again: nothing is committed or sent
     * again, each record is audited as a duplicate, and nothing is flushed.
     *
     * @throws InterruptedException if interrupted
     */
    @Test
    void dropsDuplicates() throws InterruptedException
    {
        worker("pod-a", processor()).handle(batch(3));
        int sent = output.sent.size();
        StageTestSupport.TestProcessor again = processor();
        assertThat(worker("pod-b", again).handle(batch(3)))
                .isEqualTo(BatchHandler.Outcome.DONE);
        assertThat(again.processed).hasValue(0);
        assertThat(committed).hasSize(3);
        assertThat(output.sent).hasSize(sent);
        assertThat(audit.names()).containsExactly("stage.item_completed",
                "record.duplicate", "record.duplicate", "record.duplicate");
        assertThat(audit.flushes()).isZero();
        assertThat(audit.attributes("record.duplicate"))
                .containsEntry("sdh.version", "v1")
                .containsEntry("sdh.stored_version", "v1")
                .containsEntry("sdh.state", "SENT");
    }

    /**
     * A record received again under another version is a duplicate, never
     * processed again (STAGES.md §14.3): the first output stays, nothing
     * goes to the dead-letter queue, and the version that differs is
     * audited, {@code record.version_differs}, with what an operator needs
     * to find the record. A later version can't stop the worker on a record
     * stored under an earlier one.
     *
     * @throws InterruptedException if interrupted
     */
    @Test
    void takesARecordUnderAnotherVersionAsADuplicateUnprocessed()
            throws InterruptedException
    {
        worker("pod-a", processor()).handle(batch(1));
        int sent = output.sent.size();
        StageTestSupport.TestProcessor changed =
                new StageTestSupport.TestProcessor(2, committed);
        assertThat(worker("pod-b", changed).handle(batch(1)))
                .isEqualTo(BatchHandler.Outcome.DONE);
        assertThat(changed.processed).hasValue(0);
        assertThat(committed).hasSize(1);
        assertThat(output.sent).hasSize(sent);
        assertThat(audit.names()).containsSubsequence(
                "record.version_differs", "record.duplicate");
        assertThat(audit.names()).doesNotContain("record.compare_started",
                "record.conflict");
        assertThat(audit.flushes()).isZero();
        assertThat(audit.attributes("record.version_differs"))
                .containsEntry("sdh.stage", "convert")
                .containsEntry("sdh.item_id", item.toString())
                .containsEntry("sdh.seq", 0L)
                .containsEntry("sdh.broker_message_id",
                        StageTestSupport.record(item, 0).messageId())
                .containsEntry("sdh.version", "v2")
                .containsEntry("sdh.stored_version", "v1")
                .containsEntry("sdh.state", "SENT");
    }

    /**
     * A send that fails leaves the records stored but unsent: the batch is
     * tried again, sends them (again, for those the broker took) without
     * committing them again, and then completes the item.
     *
     * @throws InterruptedException if interrupted
     */
    @Test
    void sendsAgainWhatWasStoredButNotSent() throws InterruptedException
    {
        StageWorker<String> worker = worker("pod-a", processor());
        output.failures.set(1);
        assertThat(worker.handle(batch(3)))
                .isEqualTo(BatchHandler.Outcome.RETRY);
        assertThat(committed).hasSize(3);
        assertThat(sentItems()).isEmpty();

        assertThat(worker.handle(batch(3)))
                .isEqualTo(BatchHandler.Outcome.DONE);
        assertThat(committed).hasSize(3);
        assertThat(sentRecords()).containsExactlyInAnyOrderElementsOf(
                committed);
        assertThat(sentItems()).hasSize(1);

        // Another worker that receives them later finds them sent.
        int sent = output.sent.size();
        worker("pod-b", processor()).handle(batch(3));
        assertThat(output.sent).hasSize(sent);
    }

    /**
     * Another worker's redelivery of a record stored but never sent sends
     * it, as the worker that stored it would have.
     *
     * @throws InterruptedException if interrupted
     */
    @Test
    void anotherWorkerSendsWhatWasNeverSent() throws InterruptedException
    {
        output.failures.set(1);
        worker("pod-a", processor()).handle(batch(2));
        assertThat(worker("pod-b", processor()).handle(batch(2)))
                .isEqualTo(BatchHandler.Outcome.DONE);
        assertThat(committed).hasSize(2);
        assertThat(sentRecords()).containsExactlyInAnyOrderElementsOf(
                committed);
        assertThat(sentItems()).hasSize(1);
    }

    /**
     * A record another worker holds is waited for, after the rest of the
     * batch is stored and sent, and taken over once that claim expires,
     * as a second attempt.
     *
     * @throws InterruptedException if interrupted
     */
    @Test
    void waitsForAnotherWorkersClaim() throws InterruptedException
    {
        StageRegistry stalled = StageTestSupport.registry(schema, "pod-z");
        String held = "rec-" + item + "-1";
        stalled.claim(held, item, Duration.ofMillis(500));
        StageTestSupport.TestProcessor processor = processor();
        long start = System.nanoTime();
        assertThat(worker("pod-a", processor).handle(batch(3)))
                .isEqualTo(BatchHandler.Outcome.DONE);
        assertThat(Duration.ofNanos(System.nanoTime() - start))
                .isGreaterThanOrEqualTo(Duration.ofMillis(400));
        assertThat(committed).hasSize(3);
        assertThat(committed.get(2)).isEqualTo(held);
        assertThat(processor.attempts).containsEntry(held, 2);
        assertThat(sentItems()).hasSize(1);
    }

    /**
     * Two workers given the same batch at once store each record once,
     * and between them send every record that passed and report the item
     * in progress, its failures open.
     *
     * @throws Exception if a worker fails
     */
    @Test
    void twoWorkersAtOnceStoreEachRecordOnce() throws Exception
    {
        List<BrokerMessage> batch = batch(40);
        List<StageWorker<String>> both = List.of(
                worker("pod-a", processor()), worker("pod-b", processor()));
        ExecutorService pool = Executors.newFixedThreadPool(2);
        try
        {
            List<Future<BatchHandler.Outcome>> outcomes = new ArrayList<>();
            for (StageWorker<String> worker : both)
            {
                outcomes.add(pool.submit(() -> worker.handle(batch)));
            }
            for (Future<BatchHandler.Outcome> outcome : outcomes)
            {
                assertThat(outcome.get())
                        .isEqualTo(BatchHandler.Outcome.DONE);
            }
        }
        finally
        {
            pool.shutdownNow();
        }
        assertThat(committed).hasSize(40).doesNotHaveDuplicates();
        // Every record but the 8 that failed, which are in the fail store.
        assertThat(sentRecords()).hasSize(32);
        assertThat(committed).containsAll(sentRecords());
        assertThat(sentItems()).isEmpty();
        assertThat(sentProgress()).isNotEmpty().allSatisfy(progress ->
        {
            assertThat(progress.recordCount()).isEqualTo(40);
            assertThat(progress.openCount()).isEqualTo(8);
        });
        assertThat(StageTestSupport.registry(schema, "pod-a").failureStore()
                .counts(item)).isEqualTo(new FailureStore.Counts(8, 0, 0));
    }

    /**
     * A processor that fails holds the batch back to be tried again, and
     * the retry completes it.
     *
     * @throws InterruptedException if interrupted
     */
    @Test
    void triesAgainWhenTheProcessorFails() throws InterruptedException
    {
        StageTestSupport.TestProcessor processor = processor();
        processor.failures.set(1);
        StageWorker<String> worker = worker("pod-a", processor);
        assertThat(worker.handle(batch(3)))
                .isEqualTo(BatchHandler.Outcome.RETRY);
        assertThat(committed).isEmpty();
        assertThat(audit.names()).containsExactly("stage.batch_failed");
        assertThat(worker.handle(batch(3)))
                .isEqualTo(BatchHandler.Outcome.DONE);
        assertThat(committed).hasSize(3);
    }

    /**
     * A worker closed with records claimed but not stored releases them:
     * another worker takes them over at once, as second attempts, rather
     * than after the claims' 30 seconds.
     *
     * @throws InterruptedException if interrupted
     */
    @Test
    void releasesItsClaimsWhenClosed() throws InterruptedException
    {
        StageTestSupport.TestProcessor failing = processor();
        failing.failures.set(1);
        StageWorker<String> stopping = worker("pod-a", failing);
        assertThat(stopping.handle(batch(3)))
                .isEqualTo(BatchHandler.Outcome.RETRY);
        stopping.close();
        StageTestSupport.TestProcessor processor = processor();
        long start = System.nanoTime();
        assertThat(worker("pod-b", processor).handle(batch(3)))
                .isEqualTo(BatchHandler.Outcome.DONE);
        assertThat(Duration.ofNanos(System.nanoTime() - start))
                .isLessThan(Duration.ofSeconds(10));
        assertThat(committed).hasSize(3);
        assertThat(processor.attempts.values()).containsOnly(2);
    }

    /**
     * Once the item-complete message is sent, the processor's step gets the
     * message that gave the total; if the step fails, the batch is tried
     * again and the item isn't marked complete until it succeeds.
     *
     * @throws InterruptedException if interrupted
     */
    @Test
    void runsTheItemCompletedStepWithTheTotal() throws InterruptedException
    {
        StageTestSupport.TestProcessor processor = processor();
        processor.completedFailures.set(1);
        StageWorker<String> worker = worker("pod-a", processor);
        assertThat(worker.handle(batch(2)))
                .isEqualTo(BatchHandler.Outcome.RETRY);
        assertThat(processor.completed).isEmpty();
        assertThat(sentItems()).hasSize(1);

        assertThat(worker.handle(batch(2)))
                .isEqualTo(BatchHandler.Outcome.DONE);
        assertThat(processor.completed).singleElement().satisfies(done ->
        {
            assertThat(done.done()).isEqualTo(2);
            assertThat(done.total()).isInstanceOfSatisfying(
                    ItemConverted.class, total ->
                    {
                        assertThat(total.itemId()).isEqualTo(item);
                        assertThat(total.recordCount()).isEqualTo(2);
                    });
        });
        assertThat(sentItems()).hasSize(2);
        assertThat(worker.handle(batch(2)))
                .isEqualTo(BatchHandler.Outcome.DONE);
        assertThat(processor.completed).hasSize(1);
    }

    /**
     * An interrupted generation's end is an open total (STAGES.md
     * §15.8.6): its records are counted, but no item end is sent and the
     * processor's steps don't run. The newer generation's end completes
     * the item, and, the item never reported before, is its first report,
     * so the processor's step runs for it (§15.8.7).
     *
     * @throws InterruptedException if interrupted
     */
    @Test
    void takesAnInterruptedGenerationsEndAsAnOpenTotal()
            throws InterruptedException
    {
        StageTestSupport.TestProcessor processor = processor();
        StageWorker<String> worker = worker("pod-a", processor);
        List<BrokerMessage> first = new ArrayList<>(List.of(
                StageTestSupport.record(item, 0),
                StageTestSupport.record(item, 1)));
        first.add(StageTestSupport.received(splitEnd(2, 1, true)));

        assertThat(worker.handle(first))
                .isEqualTo(BatchHandler.Outcome.DONE);

        assertThat(sentItems()).isEmpty();
        assertThat(sentProgress()).isEmpty();
        assertThat(processor.completed).isEmpty();
        assertThat(processor.progressed).isEmpty();
        assertThat(audit.attributes("item.progress"))
                .containsEntry("sdh.result", "INTERRUPTED");

        Lineage second = Lineage.of(2, List.of());
        List<BrokerMessage> next = new ArrayList<>();
        for (int seq = 0; seq < 3; seq++)
        {
            next.add(StageTestSupport.received(new RecordStored("rec-" + item
                    + "-" + seq, item, seq, seq * 10L, 10, "csv",
                    "application/json", "csv-row/1", 10, 1,
                    StageTestSupport.REF, null, null, null, second, false)));
        }
        next.add(StageTestSupport.received(splitEnd(3, 2, false)));

        assertThat(worker.handle(next)).isEqualTo(BatchHandler.Outcome.DONE);

        assertThat(sentItems()).singleElement().satisfies(done ->
                assertThat(done.recordCount()).isEqualTo(3));
        assertThat(processor.completed).singleElement().satisfies(done ->
                assertThat(done.generation()).isEqualTo(2));
    }

    /**
     * The split stage's end of a generation of this test's item.
     *
     * @param records     how many records it cut
     * @param generation  the generation
     * @param interrupted whether a newer one stopped it
     * @return the message
     */
    ConnectorMessage.ItemSplit splitEnd(long records, int generation,
            boolean interrupted)
    {
        return new ConnectorMessage.ItemSplit(item, "claims", records, 0, 99,
                "digest", null, null, null, Instant.EPOCH, Instant.EPOCH,
                List.of(), 1, generation, interrupted);
    }

    /**
     * A message that can't be trusted is dropped, one of another type is
     * acknowledged unprocessed, one of a later major version holds the
     * batch back, and one of an earlier major version is rejected and
     * acknowledged.
     *
     * @throws InterruptedException if interrupted
     */
    @Test
    void dropsUntrustedMessagesAndWaitsForAnUpgrade()
            throws InterruptedException
    {
        StageWorker<String> worker = worker("pod-a", processor());
        BrokerMessage forged = new BrokerMessage(
                "not.a.message".getBytes(US_ASCII), "x", Map.of());
        BrokerMessage other = StageTestSupport.received(new ItemLanded(item,
                "claims", "file", 1, "digest", Instant.EPOCH,
                StageTestSupport.REF));
        assertThat(worker.handle(List.of(forged, other,
                StageTestSupport.record(item, 0))))
                .isEqualTo(BatchHandler.Outcome.DONE);
        assertThat(committed).hasSize(1);
        assertThat(audit.names()).containsExactly("stage.message_rejected",
                "stage.message_unwanted");

        Map<String, Object> later = PayloadCodec.toJson(
                (ConnectorMessage) new ItemConverted(item, "convert", 1, 0,
                        Instant.EPOCH));
        later.put("v", (ContractVersion.MAJOR + 1) + ".0");
        BrokerMessage newer = new BrokerMessage(
                StageTestSupport.upstreamSealer().seal(later), "y",
                Map.of());
        assertThat(worker.handle(List.of(newer)))
                .isEqualTo(BatchHandler.Outcome.RETRY);
        assertThat(audit.names()).endsWith("stage.unreadable_version");

        // An earlier major version, which no upgrade reads, is rejected,
        // so the records behind it go on.
        Map<String, Object> earlier = PayloadCodec.toJson(
                (ConnectorMessage) new ItemConverted(item, "convert", 1, 0,
                        Instant.EPOCH));
        earlier.put("v", (ContractVersion.MAJOR - 1) + ".4");
        BrokerMessage older = new BrokerMessage(
                StageTestSupport.upstreamSealer().seal(earlier), "z",
                Map.of());
        assertThat(worker.handle(List.of(older,
                StageTestSupport.record(item, 1))))
                .isEqualTo(BatchHandler.Outcome.DONE);
        assertThat(committed).hasSize(2);
        assertThat(audit.names()).endsWith("stage.message_rejected");
    }
}
