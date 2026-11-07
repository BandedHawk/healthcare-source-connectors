/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.stage;

import static java.nio.charset.StandardCharsets.US_ASCII;
import static org.assertj.core.api.Assertions.assertThat;

import com.sdh.healthcare.connector.broker.receive.BatchHandler;
import com.sdh.healthcare.connector.broker.receive.BrokerMessage;
import com.sdh.healthcare.connector.contract.ContractVersion;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage.ItemConverted;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage.ItemLanded;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage.RecordConflict;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage.RecordStored;
import com.sdh.healthcare.connector.contract.message.PayloadCodec;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
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

    /** The dead-letter queue, shared by all workers. */
    final StageTestSupport.RecordingPublisher deadLetters =
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
                StageTestSupport.stageSealer(), output, deadLetters,
                SETTINGS, Clock.systemUTC());
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
     * A batch is processed, committed once, sent, and its item completed
     * with its counts.
     *
     * @throws InterruptedException if interrupted
     */
    @Test
    void storesSendsAndCompletesAnItem() throws InterruptedException
    {
        StageWorker<String> worker = worker("pod-a", processor());
        assertThat(worker.handle(batch(5)))
                .isEqualTo(BatchHandler.Outcome.DONE);
        assertThat(committed).hasSize(5).doesNotHaveDuplicates();
        assertThat(sentRecords()).containsExactlyInAnyOrderElementsOf(
                committed);
        assertThat(sentItems()).singleElement().satisfies(done ->
        {
            assertThat(done.itemId()).isEqualTo(item);
            assertThat(done.recordCount()).isEqualTo(5);
            assertThat(done.failedRecordCount()).isEqualTo(1);
        });
        assertThat(audit.names()).containsExactly("stage.item_completed");
        assertThat(audit.attributes("stage.item_completed"))
                .containsEntry("sdh.stage", "test")
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
     * A record received again under another version, whose output is the
     * same, is a duplicate once processed again. Before that, the worker
     * announces the comparison with what an operator needs to recover by
     * hand, and flushes it, since processing again may stop the process.
     *
     * @throws InterruptedException if interrupted
     */
    @Test
    void announcesAComparisonUnderAnotherVersion() throws InterruptedException
    {
        worker("pod-a", processor()).handle(batch(1));
        StageTestSupport.TestProcessor changed =
                new StageTestSupport.TestProcessor(2, committed)
                {
                    @Override
                    public StageOutput<String> process(
                            ConnectorMessage message, int attempt)
                    {
                        // A build that changed nothing in the output.
                        StageOutput<String> output =
                                super.process(message, attempt);
                        return new StageOutput<>(
                                output.digest().replace("v2:", "v1:"),
                                output.failed(), output.value());
                    }
                };
        assertThat(worker("pod-b", changed).handle(batch(1)))
                .isEqualTo(BatchHandler.Outcome.DONE);
        assertThat(changed.processed).hasValue(1);
        assertThat(deadLetters.sent).isEmpty();
        assertThat(audit.names()).containsSubsequence(
                "record.compare_started", "record.duplicate");
        assertThat(audit.flushes()).isEqualTo(1);
        assertThat(audit.attributes("record.compare_started"))
                .containsEntry("sdh.stage", "test")
                .containsEntry("sdh.item_id", item.toString())
                .containsEntry("sdh.seq", 0L)
                .containsEntry("sdh.broker_message_id",
                        StageTestSupport.record(item, 0).messageId())
                .containsEntry("sdh.version", "v2")
                .containsEntry("sdh.stored_version", "v1")
                .containsEntry("sdh.attempts", 1L)
                .containsEntry("sdh.state", "SENT");
    }

    /**
     * A record stored at its third attempt, received again under another
     * version, is processed again at that attempt, so an output decided by
     * attempts (the convert stage's {@code CONVERT_SKIPPED}) comes out the
     * same rather than doing the work that stopped two workers.
     *
     * @throws InterruptedException if interrupted
     */
    @Test
    void comparesAtTheAttemptItWasStoredAt() throws InterruptedException
    {
        String held = "rec-" + item + "-0";
        StageTestSupport.registry(schema, "pod-y").claim(held, item,
                Duration.ofMillis(1));
        Thread.sleep(20);
        StageTestSupport.registry(schema, "pod-z").claim(held, item,
                Duration.ofMillis(1));
        Thread.sleep(20);
        StageTestSupport.TestProcessor first = processor();
        worker("pod-a", first).handle(batch(1));
        assertThat(first.attempts).containsEntry(held, 3);

        StageTestSupport.TestProcessor changed =
                new StageTestSupport.TestProcessor(2, committed);
        worker("pod-b", changed).handle(batch(1));
        assertThat(changed.attempts).containsEntry(held, 3);
        assertThat(audit.attributes("record.compare_started"))
                .containsEntry("sdh.attempts", 3L);
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
     * A record that comes again with a different output keeps its first:
     * the second is reported to the dead-letter queue, with both digests.
     *
     * @throws InterruptedException if interrupted
     */
    @Test
    void reportsAConflict() throws InterruptedException
    {
        worker("pod-a", processor()).handle(batch(1));
        StageWorker<String> changed = worker("pod-b",
                new StageTestSupport.TestProcessor(2, committed));
        assertThat(changed.handle(batch(1)))
                .isEqualTo(BatchHandler.Outcome.DONE);
        assertThat(committed).hasSize(1);
        assertThat(deadLetters.opened()).singleElement()
                .isInstanceOfSatisfying(RecordConflict.class, conflict ->
                {
                    assertThat(conflict.recordId()).isEqualTo(
                            committed.get(0));
                    assertThat(conflict.stage()).isEqualTo("test");
                    assertThat(conflict.storedDigest()).startsWith("v1:");
                    assertThat(conflict.newDigest()).startsWith("v2:");
                });
        assertThat(audit.names()).containsSubsequence(
                "record.compare_started", "record.conflict");
        assertThat(audit.attributes("record.conflict"))
                .containsEntry("sdh.version", "v2")
                .containsEntry("sdh.stored_version", "v1");
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
     * and between them send every record and complete the item.
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
        assertThat(sentRecords()).containsAll(committed);
        assertThat(sentItems()).isNotEmpty().allSatisfy(done ->
                assertThat(done.recordCount()).isEqualTo(40));
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
                (ConnectorMessage) new ItemConverted(item, "test", 1, 0,
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
                (ConnectorMessage) new ItemConverted(item, "test", 1, 0,
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
